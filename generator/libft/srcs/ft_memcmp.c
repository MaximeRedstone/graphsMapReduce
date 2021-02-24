/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_memcmp.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 14:52:28 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 19:15:21 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

int		ft_memcmp(const void *s1, const void *s2, size_t n)
{
	const unsigned char		*cpy_s1;
	const unsigned char		*cpy_s2;

	if (!n)
		return (0);
	cpy_s1 = (const unsigned char *)s1;
	cpy_s2 = (const unsigned char *)s2;
	while (*cpy_s1 == *cpy_s2 && n > 1)
	{
		cpy_s1++;
		cpy_s2++;
		n--;
	}
	return (*cpy_s1 - *cpy_s2);
}
