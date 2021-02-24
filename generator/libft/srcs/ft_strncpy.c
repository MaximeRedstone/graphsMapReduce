/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strncpy.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 15:29:30 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/06 22:03:49 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

char	*ft_strncpy(char *dst, const char *src, size_t len)
{
	unsigned int l;

	l = 0;
	while (src[l] != '\0' && l < len)
	{
		dst[l] = src[l];
		l++;
	}
	while (l < len)
	{
		dst[l] = '\0';
		l++;
	}
	return (dst);
}
