/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_memcpy.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/10/08 18:06:02 by sikpenou          #+#    #+#             */
/*   Updated: 2019/10/21 14:28:15 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

void	*ft_memcpy(void *dst, const void *src, size_t n)
{
	long	*cpy_dst;
	long	*cpy_src;
	size_t	stop;

	cpy_dst = (long *)dst;
	cpy_src = (long *)src;
	if (n >= sizeof(long))
	{
		stop = n / sizeof(long);
		while (stop--)
			*cpy_dst++ = *cpy_src++;
		n %= sizeof(long);
	}
	while (n--)
		*(((char *)cpy_dst) + n) = *(((char *)cpy_src) + n);
	return (dst);
}
