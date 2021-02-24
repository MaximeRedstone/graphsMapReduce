/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_memccpy.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/04 18:26:19 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/06 20:37:33 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

void	*ft_memccpy(void *dst, const void *src, int c, size_t n)
{
	unsigned int		pos;
	unsigned char		*cpy_dest;
	unsigned char		*cpy_src;

	pos = 0;
	cpy_dest = (unsigned char *)dst;
	cpy_src = (unsigned char *)src;
	while (pos < n)
	{
		cpy_dest[pos] = cpy_src[pos];
		if (cpy_src[pos++] == (unsigned char)c)
			return (cpy_dest + pos);
	}
	return (NULL);
}
